# heartbeat-memo-cas

Switch: `HeartbeatMemoUsesCas = TRUE`. Expected: `HeartbeatAndMemoNeverCas` violated (exit 12).

Trace: worker 1 takes the broken `BrokenMemoCas` step, which persists its memo
snapshot with `lastMaint.mode = "CasVersion"` instead of `"Overwrite"`. The
invariant `lastMaint.class \in {"heartbeat","memo"} => lastMaint.mode = "Overwrite"`
fails on that single step.

Why it matters: `sys/maintain/workers/<id>` and `sys/maintain/memo/<id>` are
self-owned keys; a CAS on a self-owned key buys nothing and can wedge a worker
against its own stale token. The shipped writes are unconditional Overwrites.
